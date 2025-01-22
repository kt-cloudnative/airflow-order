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
                , WORKFLOW_NAME='PP_CSNG666B',WORKFLOW_ID='4fa63891e1284cd78e0a906a006653a7', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG666B-0.0.prd-tz.4.2'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 21, 21, 11, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('4fa63891e1284cd78e0a906a006653a7')

    csng666bJob_vol = []
    csng666bJob_volMnt = []
    csng666bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng666bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng666bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng666bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng666bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng666bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '5c490c5e64a6400491e9f7be8974fa36',
        'volumes': csng666bJob_vol,
        'volume_mounts': csng666bJob_volMnt,
        'env_from':csng666bJob_env,
        'task_id':'csng666bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.3',
        'arguments':["--job.name=csng666bJob", "fromDate=20240529", "toDate=20240529", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('4fa63891e1284cd78e0a906a006653a7')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng666bJob,
        Complete
    ]) 

    # authCheck >> csng666bJob >> Complete
    workflow








