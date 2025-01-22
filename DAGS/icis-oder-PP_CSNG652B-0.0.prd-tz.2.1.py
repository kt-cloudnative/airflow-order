from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='PP_CSNG652B',WORKFLOW_ID='c67fc917f89546cab6b334a8ed9fdb29', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG652B-0.0.prd-tz.2.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 19, 22, 11, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('c67fc917f89546cab6b334a8ed9fdb29')

    csng652bJob_vol = []
    csng652bJob_volMnt = []
    csng652bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng652bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng652bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng652bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng652bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng652bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '5cd9ad32b10541558d9e0e6e2e01655e',
        'volumes': csng652bJob_vol,
        'volume_mounts': csng652bJob_volMnt,
        'env_from':csng652bJob_env,
        'task_id':'csng652bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.3',
        'arguments':["--job.name=csng652bJob","fromDate=20240528", "toDate=20240528", "ver="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('c67fc917f89546cab6b334a8ed9fdb29')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng652bJob,
        Complete
    ]) 

    # authCheck >> csng652bJob >> Complete
    workflow








