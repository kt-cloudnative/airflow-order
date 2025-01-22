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
                , WORKFLOW_NAME='PP_CSNG359B',WORKFLOW_ID='c5543ef8c702476d9e87315c2a41c9c7', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG359B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 3, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('c5543ef8c702476d9e87315c2a41c9c7')

    csng359bJob_vol = []
    csng359bJob_volMnt = []
    csng359bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csng359bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csng359bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng359bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng359bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng359bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'c87a790424184d2aa8d5825732f46ef7',
        'volumes': csng359bJob_vol,
        'volume_mounts': csng359bJob_volMnt,
        'env_from':csng359bJob_env,
        'task_id':'csng359bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.67',
        'arguments':["--job.name=csng359bJob"
,"requestDate="+str(datetime.now())
,"regDate="+str(datetime.now().strftime("%Y%m%d"))
,"pstnFlag=1"
,"inDelFlag=Y"
,"inputFile=KT11202405.dat"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('c5543ef8c702476d9e87315c2a41c9c7')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng359bJob,
        Complete
    ]) 

    # authCheck >> csng359bJob >> Complete
    workflow








