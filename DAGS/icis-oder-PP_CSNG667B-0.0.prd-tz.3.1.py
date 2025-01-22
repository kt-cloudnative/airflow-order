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
                , WORKFLOW_NAME='PP_CSNG667B',WORKFLOW_ID='391c5f26b6aa4aadaced073f0e4291a3', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG667B-0.0.prd-tz.3.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 19, 21, 11, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('391c5f26b6aa4aadaced073f0e4291a3')

    csng667bJob_vol = []
    csng667bJob_volMnt = []
    csng667bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng667bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng667bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng667bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng667bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng667bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '62a8c3ec55f94022b13050152e09da5a',
        'volumes': csng667bJob_vol,
        'volume_mounts': csng667bJob_volMnt,
        'env_from':csng667bJob_env,
        'task_id':'csng667bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.69',
        'arguments':["--job.name=csng667bJob", "fromDate=20240501", "toDate=20240528", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('391c5f26b6aa4aadaced073f0e4291a3')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng667bJob,
        Complete
    ]) 

    # authCheck >> csng667bJob >> Complete
    workflow








